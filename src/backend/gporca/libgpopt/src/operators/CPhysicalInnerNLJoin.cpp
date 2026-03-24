//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CPhysicalInnerNLJoin.cpp
//
//	@doc:
//		Implementation of inner nested-loops join operator
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalInnerNLJoin.h"

#include "gpos/base.h"

#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecNonReplicated.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecReplicated.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/base/CColRefSetIter.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerNLJoin::CPhysicalInnerNLJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalInnerNLJoin::CPhysicalInnerNLJoin(CMemoryPool *mp)
	: CPhysicalNLJoin(mp)
{
	// Inner NLJ creates two distribution requests for children:
	// (0) Outer child is requested for ANY distribution, and inner child is requested for a Replicated (or a matching) distribution
	// (1) Outer child is requested for Replicated distribution, and inner child is requested for Non-Singleton

	SetDistrRequests(2);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerNLJoin::~CPhysicalInnerNLJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalInnerNLJoin::~CPhysicalInnerNLJoin() = default;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalInnerNLJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child;
//		this function creates two distribution requests:
//
//		(0) Outer child is requested for ANY distribution, and inner child is
//		  requested for a Replicated (or a matching) distribution,
//		  this request is created by calling CPhysicalJoin::PdsRequired()
//
//		(1) Outer child is requested for Replicated distribution, and inner child
//		  is requested for Non-Singleton (or Singleton if outer delivered Universal distribution)
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalInnerNLJoin::PdsRequired(CMemoryPool *mp GPOS_UNUSED,
								  CExpressionHandle &exprhdl GPOS_UNUSED,
								  CDistributionSpec *,	//pdsRequired,
								  ULONG child_index GPOS_UNUSED,
								  CDrvdPropArray *pdrgpdpCtxt GPOS_UNUSED,
								  ULONG	 // ulOptReq
) const
{
	GPOS_RAISE(
		CException::ExmaInvalid, CException::ExmiInvalid,
		GPOS_WSZ_LIT(
			"PdsRequired should not be called for CPhysicalInnerNLJoin"));
	return nullptr;
}

static CColRef* GetColRefByName(const CColRefSet *pcrs, const CWStringBase *pstrName)
{
    CColRefSetIter crsi(*pcrs);
    while (crsi.Advance())
    {
        if (crsi.Pcr()->Name().Pstr()->Equals(pstrName))
        {
            return crsi.Pcr();
        }
    }
    return nullptr;
}

CEnfdDistribution *
CPhysicalInnerNLJoin::Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  CReqdPropPlan *prppInput, ULONG child_index,
						  CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
{
	GPOS_ASSERT(2 > child_index);
	GPOS_ASSERT(ulOptReq < UlDistrRequests());

	CEnfdDistribution::EDistributionMatching dmatch =
		Edm(prppInput, child_index, pdrgpdpCtxt, ulOptReq);
	CDistributionSpec *const pdsRequired = prppInput->Ped()->PdsRequired();

	// if expression has to execute on a single host then we need a gather
	if (exprhdl.NeedsSingletonExecution())
	{
		return GPOS_NEW(mp) CEnfdDistribution(
			PdsRequireSingleton(mp, exprhdl, pdsRequired, child_index), dmatch);
	}

	if (exprhdl.HasOuterRefs())
	{
		if (CDistributionSpec::EdtSingleton == pdsRequired->Edt() ||
			CDistributionSpec::EdtStrictReplicated == pdsRequired->Edt())
		{
			return GPOS_NEW(mp) CEnfdDistribution(
				PdsPassThru(mp, exprhdl, pdsRequired, child_index), dmatch);
		}
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp)
				CDistributionSpecReplicated(CDistributionSpec::EdtReplicated),
			CEnfdDistribution::EdmSatisfy);
	}

	COptCtxt *poctxt = COptCtxt::PoctxtFromTLS();
    CPlanHint *planhint = poctxt->GetOptimizerConfig()->GetPlanHint();

	if (planhint != nullptr)
	{
		CTableDescriptorHashSet *tables = exprhdl.DeriveTableDescriptor(child_index);
		CDistributionHint* hint = planhint->GetDistributionHint(tables);
		if (hint != nullptr)
		{
			switch (hint->GetDistributionType())
			{
				case CDistributionHint::BROADCAST:
				{

						CDistributionSpec *pds = GPOS_NEW(mp) CDistributionSpecReplicated(CDistributionSpec::EdtStrictReplicated);
						return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);

				}
				case CDistributionHint::REDISTRIBUTION:
				{
					const StringPtrArray* columns = hint->GetColumnNames();
					if(columns != nullptr){
						const auto* outputCols = exprhdl.DeriveOutputColumns(child_index);
                        CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
                        for(ULONG ul = 0; ul < columns->Size(); ++ul){
                            CColRef* pcr = GetColRefByName(outputCols, (*columns)[ul]);
                            CExpression *pexprHashKey = GPOS_NEW(mp) CExpression(
                                mp,
                                GPOS_NEW(mp) CScalarIdent(mp, pcr)
                            );
                            pdrgpexpr->Append(pexprHashKey);
                        }

                        CDistributionSpecHashed *pds = GPOS_NEW(mp) CDistributionSpecHashed(
                            pdrgpexpr,
                            true
                        );

                        return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
					}
					return GPOS_NEW(mp) CEnfdDistribution(nullptr, dmatch);
				}
				case CDistributionHint::SINGLENODE:
				{
					CDistributionSpec *pds = GPOS_NEW(mp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstSegment);
                    return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
				}
                case CDistributionHint::PASSTHROUGH:
				{
                    CDistributionSpec *pds = GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
					return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
				}
				case CDistributionHint::SENTINEL:
				{
					break;
				}
			}
		}else if(child_index == 0){
           /* if(ulOptReq == 1 && CDistributionSpec::EdtUniversal == pdsOuter->Edt()){
                //Check if inner node has an applied hint
                tables = exprhdl.DeriveTableDescriptor(child_index+1);
                hint = planhint->GetDistributionHint(tables);

                if (hint != nullptr && hint->GetDistributionType() != CDistributionHint::SINGLENODE) {
                    //Cant do Singelton - Any so just pass through instead
                    CDistributionSpec *pds = PdsPassThru(mp, exprhdl, pdsRequired, child_index);
                    return GPOS_NEW(mp) CEnfdDistribution(pds, dmatch);
                }
            }*/
		}
	}

	if (GPOS_FTRACE(EopttraceDisableReplicateInnerNLJOuterChild) ||
		0 == ulOptReq)
	{
		if (1 == child_index)
		{
			CEnfdDistribution *pEnfdHashedDistribution =
				CPhysicalJoin::PedInnerHashedFromOuterHashed(
					mp, exprhdl, dmatch, (*pdrgpdpCtxt)[0]);
			if (pEnfdHashedDistribution)
			{
				return pEnfdHashedDistribution;
			}
		}
		return CPhysicalJoin::Ped(mp, exprhdl, prppInput, child_index,
								  pdrgpdpCtxt, ulOptReq);
	}
	GPOS_ASSERT(1 == ulOptReq);

	if (0 == child_index)
	{
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp)
				CDistributionSpecReplicated(CDistributionSpec::EdtReplicated),
			dmatch);
	}

	// compute a matching distribution based on derived distribution of outer child
	CDistributionSpec *pdsOuter =
		CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
	if (CDistributionSpec::EdtUniversal == pdsOuter->Edt())
	{
		// Outer child is universal, request the inner child to be non-replicated.
		// It doesn't have to be a singleton, because inner join is deduplicated.
		return GPOS_NEW(mp) CEnfdDistribution(
			GPOS_NEW(mp) CDistributionSpecNonReplicated(), dmatch);
	}

	return GPOS_NEW(mp)
		CEnfdDistribution(GPOS_NEW(mp) CDistributionSpecNonSingleton(), dmatch);
}

// EOF
